export const ScalePlanFeatureBadge = ({feature='이 기능', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                Scale 플랜 기능
            </div>
            <div>
                <p>{feature} Scale 및 Enterprise 플랜에서 제공됩니다. 업그레이드하려면 Cloud Console의 플랜 페이지를 방문하세요.</p>
            </div>
        </div>
    )
}