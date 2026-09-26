export const ScalePlanFeatureBadge = ({feature='이 기능', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Scale 요금제 기능
            </div>
            <div>
                <p>{feature} {linking_verb_are ? '는' : '은'} Scale 및 Enterprise 요금제에서 제공됩니다. 업그레이드하려면 Cloud Console의 요금제 페이지로 이동하십시오.</p>
            </div>
        </div>
    )
}