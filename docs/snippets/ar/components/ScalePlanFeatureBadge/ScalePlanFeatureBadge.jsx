export const ScalePlanFeatureBadge = ({feature='هذه الميزة', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                ميزة باقة Scale
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'متوفرة' : 'متوفرة'} في باقتَي Scale وEnterprise. للترقية، انتقل إلى صفحة الخطط في Cloud Console.</p>
            </div>
        </div>
    )
}