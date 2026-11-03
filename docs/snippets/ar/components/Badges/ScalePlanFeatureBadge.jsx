export const ScalePlanFeatureBadge = ({feature='هذا الخيار', linking_verb_are = false}) => {
    return (
        <div className="scalePlanFeatureContainer">
            <div className="scalePlanFeatureBadge">
                ميزة ضمن خطة Scale
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'متاحة' : 'متاح'} في خطتي Scale وEnterprise. للترقية، تفضل بزيارة صفحة الخطط في Cloud Console.</p>
            </div>
        </div>
    )
}